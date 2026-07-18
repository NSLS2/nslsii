from matplotlib import animation
from matplotlib import pyplot as plt
from IPython.display import HTML
from ipywidgets import interact
from tempfile import NamedTemporaryFile
import base64


def show_image_stack(images, minmax, fontsize=18, cmap="CMRmap", zlabel=r"Intensty [ADU]", figsize=(12, 10)):
    """Show an Interactive Image Stack in an IPython Notebook

    Parameters
    ----------
    images : array_like
        Stack of images of shape (N, y, x) where N is the number of images
        to show.
    minmax : tuple
        Value for the minimum and maximum of the stack in the form
        ``(min, max)``
    fontsize : int
        Fontsize for axis labels.
    cmap : string
        Colormap to use for image (from matplotlib)
    zlabel : string
        Axis label for the color bar (z-axis)
    figsize : tuple
        Figure size (from matplotlib)


    """
    n = images.shape[0]

    def view_frame(i, vmin, vmax):
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111)

        im = ax.imshow(images[i], cmap=cmap, interpolation="none", vmin=vmin, vmax=vmax)

        cbar = fig.colorbar(im)
        cbar.ax.tick_params(labelsize=fontsize)
        cbar.set_label(zlabel, size=fontsize, weight="bold")

        ax.set_title("Frame {} Min = {} Max = {}".format(i, vmin, vmax), fontsize=fontsize, fontweight="bold")

        for item in [ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels():
            item.set_fontsize(fontsize)
            item.set_fontweight("bold")

        plt.show()

    interact(view_frame, i=(0, n - 1), vmin=minmax, vmax=minmax)


def image_stack_to_movie(images, frames=None, vmin=None, vmax=None, figsize=(6, 5), cmap="CMRmap", fps=10):
    """Convert image stack to movie and show in notebook.

    Parameters
    ----------
    images : array_like
        Stack of images to show as a movie of shape (N, y, x).
    frames : int
        Number of frames to process
    vmin : number
        Minimum value to display for ``imshow``
    vmax : number
        Maximum value to display for ``imshow``
    figsize : tuple
        Figure size for each frame
    cmap : string
        Colormap to use for plotting image
    fps : int
        Framerate for created movie

    """
    if frames is None:
        frames = images.shape[0]

    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    im = plt.imshow(images[1], vmin=vmin, vmax=vmax, cmap=cmap, interpolation="none")
    cbar = fig.colorbar(im)
    cbar.ax.tick_params(labelsize=14)
    cbar.set_label(
        r"Intensity [ADU]",
        size=14,
    )
    for item in [ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels():
        item.set_fontsize(14)
        item.set_fontweight("bold")

    def animate(i):
        im.set_array(images[i])
        ax.set_title("Frame {}".format(i), fontsize=16, fontweight="bold")
        return (im,)

    anim = animation.FuncAnimation(fig, animate, frames=frames, interval=1, blit=True)
    plt.close(anim._fig)
    # return anim.to_html5_video()
    return HTML(_anim_to_html(anim, fps))


def _anim_to_html(anim, fps):
    VIDEO_TAG = """<video autoplay loop controls>
    <source src="data:video/x-m4v;base64,{0}" type="video/mp4">
    Your browser does not support the video tag.
    </video>"""

    if not hasattr(anim, "_encoded_video"):
        with NamedTemporaryFile(suffix=".mp4") as f:
            anim.save(f.name, fps=fps, extra_args=["-vcodec", "libx264", "-pix_fmt", "yuv420p"])
            video = open(f.name, "rb").read()
        anim._encoded_video = base64.b64encode(video)
    return VIDEO_TAG.format(anim._encoded_video.decode("utf-8"))
