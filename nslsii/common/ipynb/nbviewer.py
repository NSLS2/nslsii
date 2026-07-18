from IPython.display import display, HTML

_js_callback_open = """
function callback(out){
    window.open(out.content.text,'_blank');
}
"""

_js = """
var nb = IPython.notebook;
var kernel = IPython.notebook.kernel;
kernel.execute('import subprocess, os')

nb.save_checkpoint();

function gistit(){
    var command = "print("
    command += "subprocess.run(['gist',";
    command += "'"+nb.base_url+nb.notebook_path+"'.replace('/user','/home')]";
    command += ", env=dict(os.environ, http_proxy='http://proxy:8888')";
    command += ", stdout=subprocess.PIPE).stdout.decode('utf-8').rstrip()";
    command += ".replace('gist.github.com','nbviewer.jupyter.org/gist')"
    command += ")";
    kernel.execute(command, {iopub: {output: callback}});
}

setTimeout(gistit, 20000);
"""


def notebook_to_nbviewer():
    js = _js_callback_open + _js
    html = '<script type="text/javascript">'
    html += js
    html += "</script>"
    html += "<b>nbviewer will open in a new tab in 20 seconds .....</b>"
    return display(HTML(html))
