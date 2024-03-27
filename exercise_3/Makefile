# default target, when make executed without arguments
all: venv

venv/bin/activate: requirements.txt
	python3 -m venv venv
	venv/bin/pip install -r requirements.txt

venv: venv/bin/activate

run: venv
	./venv/bin/python3 code/testing.py

clean:
	rm -rf venv
	rm -rf code/__pycache__

.PHONY: all venv run clean