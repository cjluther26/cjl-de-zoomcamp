# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run
Run docker with the python:3.12.8 image in an interactive mode, use the entrypoint bash.

What's the version of pip in the image?

```
docker run -it --entrypoint=bash python:3.12.8
pip --version
```
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
