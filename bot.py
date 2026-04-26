from pyrogram import Client, filters
import boto3
import os
import time
import traceback
import asyncio
import re
import imageio
from concurrent.futures import ThreadPoolExecutor


# TELEGRAM CONFIG
api_id = 37280263
api_hash = "3bc9f61f62dcf12c10e52fd5029aaf1e"
bot_token = "8752025289:AAH6lULEnlyzmu-VdzuN-zbh-qks0Dlyyz4"

ADMIN_ID = 8522857875


# R2 CONFIG
account_id = "b4ba1dec324f41b850385cb973a19ac1"
access_key = "d3db16f58de61d4caaaf6f24d2b6486b"
secret_key = "d37dcc18e8bf3ab21bb62fe00bccb821ddd1cc0363e3fc194a5657c1ad05693f"

bucket_name = "telegram-upload"
public_url = "https://pub-16c290ff8923491db80c271f2c8f2372.r2.dev"


# connect R2
s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="auto"
)


executor = ThreadPoolExecutor(max_workers=3)


app = Client(
    "r2bot",
    api_id=api_id,
    api_hash=api_hash,
    bot_token=bot_token
)


# safe filename
def safe_filename(name):

    name = name.lower()

    name = re.sub(r'[^a-z0-9._-]', '-', name)

    name = re.sub(r'-+', '-', name)

    return name[:120]


# progress bar
def bar(p):

    total = 20

    filled = int(p/100*total)

    return "█"*filled + "░"*(total-filled)


# thumbnail generator
def extract_thumbnail(video_path):

    try:

        reader = imageio.get_reader(video_path)

        meta = reader.get_meta_data()

        fps = meta.get("fps", 25)

        duration = meta.get("duration", 0)


        if duration > 30:
            frame_number = int(fps * 30)

        elif duration > 10:
            frame_number = int(fps * 10)

        else:
            frame_number = int(fps * 2)


        frame = reader.get_data(frame_number)

        thumb_path = video_path + "_thumb.jpg"

        imageio.imwrite(thumb_path, frame)

        reader.close()

        if os.path.exists(thumb_path):

            return thumb_path


    except Exception as e:

        print("Thumbnail error:", e)


    return None


# download progress
last_dl = 0

async def dl_progress(cur,total,msg,start):

    global last_dl

    now=time.time()

    if now-last_dl < 1.5:
        return

    last_dl = now


    percent = cur*100/total

    speed = cur/(now-start)

    eta = (total-cur)/speed if speed>0 else 0


    text=f"""

⬇ Downloading

{bar(percent)} {percent:.1f}%

{cur/1024/1024:.1f}/{total/1024/1024:.1f} MB
{speed/1024/1024:.2f} MB/s
ETA {eta:.0f}s
"""

    await msg.edit(text)


# upload part
def upload_part(data):

    part_no, chunk, key, upload_id = data

    part = s3.upload_part(
        Bucket=bucket_name,
        Key=key,
        PartNumber=part_no,
        UploadId=upload_id,
        Body=chunk
    )

    return {
        "PartNumber": part_no,
        "ETag": part["ETag"]
    }


# upload with progress
async def upload_file_r2(path,key,content_type,msg):

    size=os.path.getsize(path)

    upload=s3.create_multipart_upload(

        Bucket=bucket_name,

        Key=key,

        ContentType=content_type,

        ContentDisposition="inline",

        CacheControl="public, max-age=31536000"
    )

    upload_id=upload["UploadId"]

    parts=[]

    with open(path,"rb") as f:

        part_no=1

        while True:

            chunk=f.read(8*1024*1024)

            if not chunk:
                break

            parts.append(

                (part_no,chunk,key,upload_id)

            )

            part_no+=1


    uploaded=0

    start=time.time()

    completed=[]


    for i,res in enumerate(executor.map(upload_part,parts)):

        completed.append(res)

        uploaded+=len(parts[i][1])


        percent=uploaded*100/size

        speed=uploaded/(time.time()-start)

        eta=(size-uploaded)/speed if speed>0 else 0


        text=f"""

☁ Uploading

{bar(percent)} {percent:.1f}%

{uploaded/1024/1024:.1f}/{size/1024/1024:.1f} MB
{speed/1024/1024:.2f} MB/s
ETA {eta:.0f}s
"""

        await msg.edit(text)


    s3.complete_multipart_upload(

        Bucket=bucket_name,

        Key=key,

        UploadId=upload_id,

        MultipartUpload={"Parts":completed}
    )


async def send_error(msg,e):

    await msg.reply(

        "ERROR\n\n"
        +str(e)+
        "\n\n"+
        traceback.format_exc()[:2000]
    )


@app.on_message(filters.video | filters.document)
async def upload_video(client,msg):

    if msg.from_user.id!=ADMIN_ID:

        return


    try:

        m=await msg.reply("Starting...")


        start=time.time()

        path=await msg.download(

            progress=lambda c,t:

            client.loop.create_task(

                dl_progress(c,t,m,start)
            )
        )


        clean=safe_filename(os.path.basename(path))

        name=str(int(time.time()))+"_"+clean


        video_key=f"video/{name}"


        await m.edit("Generating thumbnail...")


        thumb=extract_thumbnail(path)


        await upload_file_r2(

            path,

            video_key,

            "video/mp4",

            m
        )


        video_link=f"{public_url}/{video_key}"


        thumb_link=None


        if thumb:

            thumb_key=f"thumb/{name}.jpg"

            await upload_file_r2(

                thumb,

                thumb_key,

                "image/jpeg",

                m
            )

            thumb_link=f"{public_url}/{thumb_key}"


        await m.delete()


        await msg.reply(

f"""Upload complete

VIDEO:
{video_link}

THUMBNAIL:
{thumb_link if thumb_link else "not generated"}
"""
)


        os.remove(path)

        if thumb:
            os.remove(thumb)


    except Exception as e:

        await send_error(msg,e)


print("BOT RUNNING")

app.run()
