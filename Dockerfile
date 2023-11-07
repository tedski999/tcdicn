FROM python
COPY example.py example.py
COPY tcdicn.py tcdicn.py
CMD ["python3", "example.py"]
