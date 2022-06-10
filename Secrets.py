import json
import sys
import secrets
import web3
import json
import getpass

class Secrets:
    private_key = None
    def __init__(self, json_file = 'secrets.json' ) -> None:
        with open(json_file) as json_file:
            self.secrets = json.load(json_file)

    def get(self, key):
        if key in self.secrets:
            return self.secrets[key]
        return None

    def read_private_key(self, json_file) -> str:
        if Secrets.private_key:
            return Secrets.private_key
        try:
            with open(json_file) as json_file:
                password = getpass.getpass(f"Please input a password: ")
                keystore = json.load(json_file)
                a = web3.eth.Account() # no provider
                Secrets.private_key = a.decrypt(keystore,password)
                return Secrets.private_key
        except Exception as e:
            print("key read failed", e)
            return None

    def create_account(self):
        nonce = secrets.token_hex(32)
        w3 = web3.Web3()
        myAccount = w3.eth.account.create(nonce)
        myAddress = myAccount.address
        myPrivateKey = myAccount.privateKey
        filename = myAccount.address + ".json"
        print('Filename will be : '+ format(filename))
        password = getpass.getpass(f"Please input a password: ")
        password1 = getpass.getpass(f"Please re-input the password: ")
        if password != password1:
            print("Passwords do not match")
            sys.exit(-1)

        a = web3.eth.Account() # no provider
        keystore = a.encrypt(myAccount.privateKey,password)

        try:
            f = open(filename, 'w')
        except OSError:
            print ("Could not open/read file:", )
            sys.exit(-2)
        f.write(json.dumps(keystore))

class MockSecrets:
    def __init__(self) -> None:
        self.secrets = {
            "MAINNET_PUBLIC_KEY":"FAKEeB7a9CDe59618F4c187415641063c02F41C2",
            "FTX_API_KEY":"FAKES5pej6mI1nJdlDCOODiWrlHjwTpuenvbPhGM",
            "FTX_API_SECRET":"FAKEPz-jp4JEQ-PHJxIqO0yCfKvZPma6TDZJJBGj",
            "FTX_SUBACCOUNT":"Development",
            "FTX_ETH_DEPOSIT_ADDRESS":"0xFAKE3cc8B01C2AF12A06183939F436E3C2c5D9a7"
        }

    def get(self, key):
        if key in self.secrets:
            return self.secrets[key]
        return None
