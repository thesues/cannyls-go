curl -v -X POST http://localhost:8081/put/$1 \
  -F "file=@$2" \
  -H "Content-Type: multipart/form-data"
