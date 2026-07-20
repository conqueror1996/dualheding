import paramiko
import time

def deploy_to_vps():
    host = '152.228.227.85'
    port = 20008
    username = 'root'
    password = 'gkKIwgGUkUab4Q1f'

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print(f"Connecting to {host}:{port}...")
    client.connect(host, port=port, username=username, password=password)
    
    commands = [
        "mkdir -p /root/dualhedge",
        "cd /root/dualhedge",
        "if [ ! -d .git ]; then git clone https://github.com/conqueror1996/DUALHEDGE.git .; else git fetch --all && git reset --hard origin/main; fi",
        "apt-get update && apt-get install -y python3-pip python3-venv",
        "if [ ! -d venv ]; then python3 -m venv venv; fi",
        "source venv/bin/activate && pip install -r requirements.txt",
        "cat << 'SERVICE' > /etc/systemd/system/dualhedge.service\n[Unit]\nDescription=DualHedge Baccarat Bot\nAfter=network.target\n\n[Service]\nUser=root\nWorkingDirectory=/root/dualhedge\nExecStart=/root/dualhedge/venv/bin/python3 app.py\nRestart=always\nRestartSec=3\nEnvironment=\"PATH=/root/dualhedge/venv/bin\"\n\n[Install]\nWantedBy=multi-user.target\nSERVICE",
        "systemctl daemon-reload",
        "systemctl enable dualhedge",
        "systemctl restart dualhedge"
    ]
    
    for cmd in commands:
        print(f"Running: {cmd}")
        stdin, stdout, stderr = client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        print(stdout.read().decode())
        if exit_status != 0:
            print(f"Error: {stderr.read().decode()}")
            
    print("Deployment finished!")
    client.close()

if __name__ == '__main__':
    deploy_to_vps()
