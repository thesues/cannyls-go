wrk.method = "POST"
wrk.headers["Content-Type"] = "multipart/form-data; boundary=----WebKitFormBoundaryX3bY6PBMcxB1vCan"
bodyhead = "------WebKitFormBoundaryX3bY6PBMcxB1vCan"
bodyhead = bodyhead .. '\r\n'
bodyhead = bodyhead .. [[Content-Disposition: form-data; name="file"; filename="file"]]
bodyhead = bodyhead .. '\r\n'
bodyhead = bodyhead .. 'Content-Type: application/octet-stream'
bodyhead = bodyhead .. '\r\n'
bodyhead = bodyhead .. '\r\n'
file = io.open('4kfile',"rb")
bodyhead = bodyhead .. file:read("*a")
bodyhead = bodyhead .. '\r\n'
bodyhead = bodyhead .. '------WebKitFormBoundaryX3bY6PBMcxB1vCan--'
wrk.body   = bodyhead
io.close(file)

done = function(summary, latency, requests)
   io.write("------------------------------\n")
   for _, p in pairs({ 50, 90, 99, 99.999 }) do
      n = latency:percentile(p)
      io.write(string.format("%g%%,%d\n", p, n))
   end
end
