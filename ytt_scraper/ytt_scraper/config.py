import os


def get_youtube_credentials():
    return (
        os.environ['YOUTUBE_API_SERVICE_NAME'],
        os.environ['YOUTUBE_API_VERSION'],
        os.environ['YOUTUBE_API_KEY'],
    )


def get_model_path(model_name: str = 'DEFAULT'):
    return os.environ[f'{model_name}_MODEL_PATH']