from flask import send_file


from app import create_app

app = create_app()


@app.route("/")
def index():
    return send_file("templates/index.html")

if __name__ == "__main__":
    app.run(host='0.0.0.0')
