from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
from birds_detection import image_prediction, video_prediction
from tempfile import NamedTemporaryFile
import shutil
import os

app = FastAPI()

@app.post("/predict/image")
async def predict_image(
    image_url: str = Form(None),
    image_file: UploadFile = File(None),
):
    if not image_url and not image_file:
        return JSONResponse({"error": "Provide either image_url or upload an image_file"}, status_code=400)

    try:
        # If file uploaded
        if image_file:
            with NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                shutil.copyfileobj(image_file.file, tmp)
                tmp_path = tmp.name
            result = image_prediction(image_path=tmp_path)
            os.remove(tmp_path)

        # If URL provided
        else:
            result = image_prediction(image_path=image_url)

        return {"tags": result}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/predict/video")
async def predict_video(
    video_file: UploadFile = File(...),
):
    try:
        # Save uploaded video to temp file
        with NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            shutil.copyfileobj(video_file.file, tmp)
            tmp_path = tmp.name

        result = video_prediction(video_path=tmp_path)
        os.remove(tmp_path)

        return {"tags": result}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
