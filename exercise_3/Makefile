# default target, when make executed without arguments
all: venv

venv/bin/activate: requirements.txt
	python3 -m venv venv
	venv/bin/pip install -r requirements.txt

venv: venv/bin/activate

run: venv
	./venv/bin/python3 code/run_all.py

clean:
	rm -rf venv
	find . -type f -name '*.pyc' -delete

.PHONY: all venv run clean