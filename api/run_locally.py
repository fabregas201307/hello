from sys_eval import app

# can access it by hitting IP: http://127.0.0.1:5000/

def main():
    app.run(host="0.0.0.0", debug=True)