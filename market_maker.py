import requests
import base64
import Crypto.PublicKey.RSA

with open('mmtest_public.pem','rb') as f:
    pubkey = Crypto.PublicKey.RSA.importKey(f.read())


b64_pubkey = base64.b64encode(pubkey.exportKey(format='DER'))

payload={"users": [{"role": "admin", "authentication_methods": [{"type": "RSA", "key": "base64key", "value": ""}]}]}

print(b64_pubkey)
url = 'http://localhost:4000/api/v1/accounts'
r = requests.post(url, data=payload, timeout=1.0)
print(r.request)
