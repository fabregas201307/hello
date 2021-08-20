import os

def main():
    package_dir = os.path.dirname(os.path.abspath(__file__))
    gunicorn_path = os.path.join(package_dir, "application", "gunicorn.conf")
    cmd = f"gunicorn -c {gunicorn_path} sys_eval:app -t 3600"
    os.system(cmd)