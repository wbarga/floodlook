To install:

1. Make sure you've got git and python3 installed on your computer.
2. Create a project folder
    - call it whatever you want and put it wherever you like to keep stuff like that
3. Clone repository
    - on linux, run:
    git clone https://github.com/wbarga/floodlook
    You might need to set up a personal access token, instructions will show up if so
    On windows or macos, process is similar but google it
3. create virtual Environment
    - navigate in command line to the project folder
    - on linux, run:
        python -m venv floodvenv
    - you can call that folder whatever you want, but I called it floodvenv
    - on windows or macos, this process is similar but different. Google it
4. Activate virtual Environment
    - run source floodvenv/bin/activate
5. Rename the folder "Sample_DB" to "data"
6. Edit the sample database if you'd like
    - the "gauges" table lists all of the flood gauges that the script should read
    - I have 3 already in the sample table, that I used for development
    - if you want to check your local gauges, go to https://water.weather.gov/ahps/
        - find your local gauge that you want to read
        - edit that gauge into the gauge table. The table takes the gauge ID, which is
            usually 4 letters and a number
        - you'll need an editor for SQLite if you want a GUI. I use the one called "DB Browser for SQLite"
            that comes packaged with Ubuntu
7. Run script
    - python3 floodlook.py
