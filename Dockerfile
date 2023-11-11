FROM python
COPY *.py .
CMD exec python3 $SCRIPT
