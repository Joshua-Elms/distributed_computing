venv/bin/activate: requirements.txt # creates a virtual environment and installs the requirements
	python3 -m venv venv
	./venv/bin/pip install -r requirements.txt
	./venv/bin/python3 -m pip install --upgrade pip

server: .SILENT venv/bin/activate # activates the virtual environment then runs the server
	. venv/bin/activate
	TIME=60 # pass the time with "make server TIME=<seconds>"
	./venv/bin/python3 server.py $(TIME)

main: .SILENT venv/bin/activate # activates the virtual environment then runs the main program
	. venv/bin/activate
	./venv/bin/python3 main.py

.SILENT: # prevents commands from being echoed

.PHONY: server # prevents make from doing something with a file named server

