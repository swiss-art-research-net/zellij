An ontological toolkit

## Getting Started
Zellīj is a web app, and as such is designed to be deployed to a web server, such as (for example) PythonAnywhere.com.

## Deploy to PythonAnywhere.com
### Deploy the database
1) Log in to PythonAnywhere using the Zellīj site administrator credentials.
2) Click on the "Databases" tab.
3) Create a MySQL database.
4) Click on the `databasename$default` to open the database console.
5) Paste the contents of the file `db.sql` into the console to populate the database.

### Deploy the website
1) Log in to PythonAnywhere using the Zellīj site administrator credentials.
2) Click on the "Consoles" tab.
3) Click on "Bash" item.
4) Create a virtual environment for the python requirements by typing `mkvirtualenv venv`
5) Clone the repository
6) Type `cd Zellij`.
7) Type `pip3 install -r requirements.txt`.
8) Create and populate the .env template with the appropriate values `cp env.template .env`
9) Go to the `Web` tab and scroll to the `Code` section.
10) Update the `Source code` and `Working Directory` to point to the `Zellij` folder.
11) Press on the `WSGI configuration file` to open it.
12) Scroll to the `FLASK` section and update it to look like the following snippet:
    ```python
    # +++++++++++ FLASK +++++++++++
    # Flask works like any other WSGI-compatible framework, we just need
    # to import the application.  Often Flask apps are called "app" so we
    # may need to rename it during the import:
    #
    #
    import sys
    #
    ## The "/home/michaelgriniezakis" below specifies your home
    ## directory -- the rest should be the directory you uploaded your Flask
    ## code to underneath the home directory.  So if you just ran
    ## "git clone git@github.com/myusername/myproject.git"
    ## ...or uploaded files to the directory "myproject", then you should
    ## specify "/home/michaelgriniezakis/myproject"
    path = 'pathto/Zellij'
    if path not in sys.path:
        sys.path.append(path)

    from website.main import app as application  # noqa
    #
    # NB -- many Flask guides suggest you use a file called run.py; that's
    # not necessary on PythonAnywhere.  And you should make sure your code
    # does *not* invoke the flask development server with app.run(), as it
    # will prevent your wsgi file from working.
    ```
    and press save
14) Go back to the `Web` tab and press reload

## Running locally for development
### Precursors
1) Download your preferred IDE (mine is Eclipse; I may mention it by name later but any Python IDE will do).
2) Download this project.

### Run locally
1) Right-click on the file **main.py** and click the "Run" button in your IDE (probably looks like a green "play" button).
2) Browse to your local URL using the port your IDE tells you it's running on; i.e. 127.0.0.1:5000.

### Extra stuff available to developer
* Because this is a work in progress, error messages on the server will contain full stack trace and debug information. But because that's production, all that info is encrypted into a secure string. This string can be reported to you by users, and can be _decrypted_ by this application running locally. When running locally you'll be able to go to http://127.0.0.1:5000/errordecoder and paste in the encrypted string, and see the full error message.
