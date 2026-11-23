import hashlib
import os
import ssl
from pathlib import Path
from urllib.request import urlopen

maven = "https://repo.maven.apache.org/maven2/"
ca = Path("/run/secrets/build_ca")
context = ssl.create_default_context(cafile=os.fspath(ca) if ca.is_file() else None)
for line in Path("/tmp/spark-kafka-jars.lock").read_text().splitlines():
    expected, artifact = line.split()
    url = maven + artifact
    with urlopen(url, context=context, timeout=45) as response:
        jar = response.read()
    if hashlib.sha256(jar).hexdigest() != expected:
        raise ValueError(f"checksum mismatch for {artifact}")
    (Path("/opt/spark/jars") / Path(artifact).name).write_bytes(jar)
