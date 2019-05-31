curl -v -X POST http://47.75.120.199:8081/put/$1 \
  -F "file=@$2" \
  -H "Content-Type: multipart/form-data"
