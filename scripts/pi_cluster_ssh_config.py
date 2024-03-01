from argparse import ArgumentParser


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Generate a .ssh/config file for directly connecting onto the raspberry pis",
    )
    parser.add_argument("pi_user")
    parser.add_argument("gruenau_user")
    parser.add_argument("gruenau_hop_node")

    args = parser.parse_args()

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

    print(
        f"""
Host gruenau_hop
  HostName {args.gruenau_hop_node}.informatik.hu-berlin.de
  USER {args.gruenau_user}
  """
    )

    for name, s in pi_data.items():
        substr = s.split(" ")
        ip = substr[8].split("@")[1]
        port = substr[5]

        print(
            f"""
Host {name}
  HostName {ip}
  USER {args.pi_user}
  StrictHostKeyChecking no
  ProxyJump gruenau_hop
  Port {port}"""
        )
