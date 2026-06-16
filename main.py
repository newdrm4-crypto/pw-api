from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
import httpx
import re
import os
import tempfile
import subprocess
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor

app = FastAPI(title="PW Video Downloader API - MPD/DASH Support")

# Your PW token
PW_TOKEN = os.getenv("PW_TOKEN", "your_token_here")
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")

# For parallel downloads
executor = ThreadPoolExecutor(max_workers=10)

@app.get("/pw")
async def get_pw_video(url: str, token: Optional[str] = None):
    auth_token = token if token else PW_TOKEN
    if not auth_token or auth_token != PW_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    if not url:
        raise HTTPException(status_code=400, detail="Missing 'url' parameter")
    
    try:
        video_id = extract_video_id(url)
        if not video_id:
            raise HTTPException(status_code=400, detail="Invalid video URL")
        
        manifest_url = f"https://www.pw.live/player/video/{video_id}"
        
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/dash+xml, application/xml, */*",
                "Referer": "https://www.pw.live/"
            }
            
            response = await client.get(manifest_url, headers=headers)
            
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch video data")
            
            content_type = response.headers.get("content-type", "")
            
            if "application/dash+xml" in content_type or ".mpd" in response.text[:200] or "<MPD" in response.text[:200]:
                return await process_mpd_manifest(response.text, video_id, str(response.url))
            else:
                data = response.json()
                mpd_url = extract_mpd_from_json(data)
                if mpd_url:
                    mpd_response = await client.get(mpd_url, headers=headers)
                    if mpd_response.status_code == 200:
                        return await process_mpd_manifest(mpd_response.text, video_id, mpd_url)
                
                video_links = extract_video_links(data)
                if video_links:
                    return {
                        "status": "success",
                        "message": "Video fetched successfully (direct links)",
                        "data": {
                            "video_id": video_id,
                            "title": data.get("title", "Untitled"),
                            "download_links": video_links,
                            "thumbnail": data.get("thumbnail", ""),
                            "format": "direct"
                        }
                    }
                
                raise HTTPException(status_code=404, detail="No video links or MPD manifest found")
                
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Request timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_mpd_manifest(mpd_content: str, video_id: str, base_url: str):
    try:
        root = ET.fromstring(mpd_content)
        ns = {'': 'urn:mpeg:dash:schema:mpd:2011'}
        
        representations = []
        for adaptation_set in root.findall('.//AdaptationSet'):
            for representation in adaptation_set.findall('.//Representation'):
                if representation.get('mimeType', '').startswith('video/'):
                    bandwidth = int(representation.get('bandwidth', 0))
                    width = representation.get('width', '0')
                    height = representation.get('height', '0')
                    
                    segment_template = representation.find('.//SegmentTemplate')
                    if segment_template is not None:
                        media_url = segment_template.get('media', '')
                        initialization_url = segment_template.get('initialization', '')
                        timescale = segment_template.get('timescale', '1')
                        
                        representations.append({
                            'id': representation.get('id', ''),
                            'bandwidth': bandwidth,
                            'width': width,
                            'height': height,
                            'media_url': media_url,
                            'initialization_url': initialization_url,
                            'timescale': int(timescale),
                            'quality': f"{height}p"
                        })
        
        if not representations:
            raise HTTPException(status_code=404, detail="No video representations found in MPD")
        
        representations.sort(key=lambda x: x['bandwidth'], reverse=True)
        best_rep = representations[0]
        
        segment_count = get_segment_count(root)
        if segment_count == 0:
            segment_count = get_segment_count_from_timeline(root)
        
        if segment_count == 0:
            segment_count = 50
        
        base_mpd_url = base_url.rsplit('/', 1)[0] + '/'
        segment_urls = []
        
        for i in range(1, segment_count + 1):
            segment_url = best_rep['media_url'].replace('$Number$', str(i))
            segment_url = segment_url.replace('$RepresentationID$', best_rep['id'])
            
            if not segment_url.startswith('http'):
                segment_url = base_mpd_url + segment_url
            
            segment_urls.append(segment_url)
        
        init_url = best_rep['initialization_url']
        if init_url and not init_url.startswith('http'):
            init_url = base_mpd_url + init_url
        
        with tempfile.TemporaryDirectory() as temp_dir:
            init_path = None
            if init_url:
                init_path = os.path.join(temp_dir, "init.mp4")
                await download_file(init_url, init_path)
            
            segment_files = []
            download_tasks = []
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                for i, seg_url in enumerate(segment_urls):
                    seg_path = os.path.join(temp_dir, f"segment_{i:04d}.m4s")
                    segment_files.append(seg_path)
                    download_tasks.append(download_file_async(client, seg_url, seg_path))
                
                semaphore = asyncio.Semaphore(20)
                
                async def download_with_semaphore(url, path):
                    async with semaphore:
                        await download_file_async(client, url, path)
                
                await asyncio.gather(*[download_with_semaphore(seg_url, seg_path) 
                                     for seg_url, seg_path in zip(segment_urls, segment_files)])
            
            existing_segments = [f for f in segment_files if os.path.exists(f) and os.path.getsize(f) > 0]
            if not existing_segments:
                raise HTTPException(status_code=404, detail="Failed to download any video segments")
            
            output_filename = f"video_{video_id}_{uuid.uuid4().hex[:8]}.mp4"
            output_path = os.path.join(temp_dir, output_filename)
            
            concat_file = os.path.join(temp_dir, "concat.txt")
            with open(concat_file, "w") as f:
                if init_path and os.path.exists(init_path):
                    f.write(f"file '{init_path}'\n")
                for seg in sorted(existing_segments):
                    f.write(f"file '{seg}'\n")
            
            cmd = [
                FFMPEG_PATH,
                "-f", "concat",
                "-safe", "0",
                "-i", concat_file,
                "-c", "copy",
                "-bsf:a", "aac_adtstoasc",
                output_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                return await merge_alternative(segment_files, init_path, output_path, video_id)
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return FileResponse(
                    output_path,
                    filename=output_filename,
                    media_type="video/mp4"
                )
            else:
                raise HTTPException(status_code=500, detail="Failed to create merged video")
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MPD processing error: {str(e)}")

async def merge_alternative(segment_files: List[str], init_path: str, output_path: str, video_id: str):
    try:
        list_file = os.path.join(os.path.dirname(output_path), "filelist.txt")
        with open(list_file, "w") as f:
            if init_path and os.path.exists(init_path):
                f.write(f"file '{init_path}'\n")
            for seg in sorted(segment_files):
                if os.path.exists(seg) and os.path.getsize(seg) > 0:
                    f.write(f"file '{seg}'\n")
        
        cmd = [
            FFMPEG_PATH,
            "-f", "concat",
            "-safe", "0",
            "-i", list_file,
            "-c", "copy",
            output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0 and os.path.exists(output_path):
            return FileResponse(
                output_path,
                filename=f"video_{video_id}_{uuid.uuid4().hex[:8]}.mp4",
                media_type="video/mp4"
            )
        else:
            raise Exception("Merge failed")
    except Exception as e:
        raise Exception(f"Alternative merge failed: {str(e)}")

async def download_file_async(client: httpx.AsyncClient, url: str, path: str):
    try:
        response = await client.get(url)
        if response.status_code == 200:
            with open(path, "wb") as f:
                f.write(response.content)
            return True
        return False
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return False

async def download_file(url: str, path: str):
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url)
        if response.status_code == 200:
            with open(path, "wb") as f:
                f.write(response.content)
            return True
    return False

def get_segment_count(root: ET.Element) -> int:
    timeline = root.find('.//SegmentTimeline')
    if timeline is not None:
        max_segment = 0
        for seg in timeline.findall('S'):
            rep_count = int(seg.get('r', 0))
            if rep_count > 0:
                max_segment += rep_count + 1
            else:
                max_segment += 1
        return max_segment
    
    seg_template = root.find('.//SegmentTemplate')
    if seg_template is not None:
        duration = seg_template.get('duration')
        if duration:
            timescale = int(seg_template.get('timescale', '1'))
            adaptation = root.find('.//AdaptationSet')
            if adaptation is not None:
                total_duration = adaptation.get('duration')
                if total_duration:
                    return int(int(total_duration) / int(duration))
    
    return 0

def get_segment_count_from_timeline(root: ET.Element) -> int:
    timeline = root.find('.//SegmentTimeline')
    if timeline is not None:
        count = 0
        for seg in timeline.findall('S'):
            r = int(seg.get('r', 0))
            count += r + 1
        return count
    return 0

def extract_video_id(url: str) -> Optional[str]:
    patterns = [
        r"pw\.live/video/([a-zA-Z0-9_-]+)",
        r"pw\.live/([a-zA-Z0-9_-]+)",
        r"video/([a-zA-Z0-9_-]+)"
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def extract_mpd_from_json(data: dict) -> Optional[str]:
    for key in ["mpd_url", "dash_url", "manifest_url", "playlist_url", "stream_url", "hls_url"]:
        if key in data and data[key]:
            return data[key]
    
    for value in data.values():
        if isinstance(value, dict):
            result = extract_mpd_from_json(value)
            if result:
                return result
        elif isinstance(value, str) and (".mpd" in value.lower() or "dash" in value.lower()):
            return value
    return None

def extract_video_links(data: dict) -> dict:
    links = {}
    for key, value in data.items():
        if isinstance(value, str) and ("video" in key.lower() or ".mp4" in value.lower()):
            if value.startswith("http"):
                links[key] = value
    return links

@app.get("/")
async def root():
    return {
        "status": "active",
        "message": "PW Video Downloader API - Handles MPD/DASH chunks automatically",
        "endpoints": {
            "/pw": "Download video using URL and token"
        },
        "supported_formats": ["MPEG-DASH (.mpd)", "HLS (.m3u8)", "Direct MP4"]
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}
