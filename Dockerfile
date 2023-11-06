FROM python
RUN apt update
RUN apt install --assume-yes iproute2 tcpdump
COPY tcdicn.py tcdicn.py
CMD ["python3", "tcdicn.py"]
