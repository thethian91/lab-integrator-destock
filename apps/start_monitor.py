# Launcher neutro para PyInstaller (evita ejecutar apps/monitor/main.py como script)
def main():
    from apps.monitor.main import main as run

    run()


if __name__ == "__main__":
    main()
