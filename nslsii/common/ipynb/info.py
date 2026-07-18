import sys
import psutil
import platform
from IPython.display import HTML


def get_sys_info():
    """Display info on system and output as nice HTML"""
    html = "<h3>System Information for {}</h3>".format(platform.node())
    html += "<table>"

    html += '<tr><td align="left">Python Executable</td>'
    html += "<td>{}</td></tr>".format(sys.executable)
    html += "<tr><td>Kernel PID</td><td>{}</td></tr>".format(psutil.Process().pid)

    mem = psutil.virtual_memory()
    html += "<tr><td>Total System Memory</td>"
    html += "<td>{:.4} Mb</td></td>".format(mem.total / 1024**3)
    html += "<tr><td>Total Memory Used</td>"
    html += "<td>{:.4} Mb</td></td>".format(mem.used / 1024**3)
    html += "<tr><td>Total Memory Free</td>"
    html += "<td>{:.4} Mb</td></td>".format(mem.free / 1024**3)

    html += "<tr><td>Number of CPU Cores</td><td>{}</td></tr>".format(psutil.cpu_count())
    html += "<tr><td>Current CPU Load</td><td>{} %</td></tr>".format(psutil.cpu_percent(1, False))

    html += "</table>"
    return HTML(html)


def show_kernels():
    """Show all IPython Kernels on System"""

    total_mem = psutil.virtual_memory().total

    html = (
        "<h3>IPython Notebook Processes on {}</h3>"
        "<table><tr>"
        "<th>Username</th><th>PID</th><th>CPU Usage</th>"
        "<th>Process Memory</th><th>System Memory Used</th><th>Status</th>"
        "</tr>"
    ).format(platform.node())

    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=["pid", "username", "cmdline", "memory_info", "status"])
        except psutil.NoSuchProcess:
            pass
        else:
            if any(x in pinfo["cmdline"] for x in ["IPython.kernel", "ipykernel_launcher"]):
                html += "<tr>"
                html += "<td>{username}</td><td>{pid}</td>".format(**pinfo)
                p = psutil.Process(pinfo["pid"]).cpu_percent(0.1)
                html += "<td>{}%</td>".format(p)
                html += "<td>{:.4} Mb</td>".format(pinfo["memory_info"].vms / 1024**3)
                html += "<td>{:.3}%</td>".format(100 * pinfo["memory_info"].vms / total_mem)
                html += "<td>{}</td>".format(pinfo["status"])
                html += "</tr>"

    html += "</table>"
    return HTML(html)
