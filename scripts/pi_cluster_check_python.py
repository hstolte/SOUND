from argparse import ArgumentParser


pi_data = {
    "pi1": "sshpass -p 94855822 ssh -p 2235 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi2": "sshpass -p 11078954 ssh -p 2295 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi3": "sshpass -p 45687231 ssh -p 2277 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi4": "sshpass -p 35468432 ssh -p 2252 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi5": "sshpass -p 33987654 ssh -p 2289 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi6": "sshpass -p 11241310 ssh -p 2215 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi7": "sshpass -p 87504840 ssh -p 2203 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi8": "sshpass -p 84100841 ssh -p 2288 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi9": "sshpass -p 68186105 ssh -p 2299 -o StrictHostKeyChecking=no pi@141.20.27.118",
    "pi10": "sshpass -p 42069421 ssh -p 2223 -o StrictHostKeyChecking=no pi@141.20.27.118",
}


if __name__ == "__main__":
    for name, command in pi_data.items():
        print(f'echo "{"="*50}{name}"')
        print(command, r' "cd /usr/bin && ls python*"')
