"""
Created on Mar. 11, 2021

@author: Pete Harris
"""
import os
import dotenv
from socket import gethostname
from flask import Flask


dotenv.load_dotenv()


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY="MQCpsXZBNm3cXtbFQ3y6g6ZA",
        SYMMETRIC_KEYFILE=os.path.join(app.instance_path, "secretkeyfile.bytes"),
        DATABASE_USER="root",
        DATABASE_PASSWORD="",
        DATABASE_NAME="zellij$website",
        DATABASE_HOST="localhost",
        APP_PROJECT_FOLDER="Zellij",
        ERROR_OBFUSCATOR_SYMMETRIC_KEY=b"eRrOr-oBfUsCaToR-FoR-PrOdUcTiOn!",
        UPLOAD_FOLDER="../uploads",
        ALLOWED_UPLOAD_EXTENSIONS={"txt", "rdf", "turtle"},
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py", silent=True)
    else:
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    from website import db

    db.init_app(app)

    from website import auth

    app.register_blueprint(auth.bp)

    from website import datasources

    app.register_blueprint(datasources.bp)

    from website import docs

    app.register_blueprint(docs.bp)

    from website import tools

    app.register_blueprint(tools.bp)

    from website import pages

    app.register_blueprint(pages.bp)
    app.add_url_rule("/", endpoint="index")

    from website import error

    app.register_blueprint(error.bp)

    return app


if __name__ == "__main__":
    # from website import db
    # db.init_db()    # might want this for database in the future, for running on PythonAnywhere from their bash script.
    if "liveconsole" not in gethostname():
        # this won't run even from console if run from PythonAnywhere console (named 'liveconsole'),
        # so this script can be used to just activate DB in previous line.
        app = create_app()
        from website import errordecode

        app.register_blueprint(errordecode.bp)
        from website import testpages

        app.register_blueprint(testpages.bp)
        app.run()
    else:
        app = create_app()
