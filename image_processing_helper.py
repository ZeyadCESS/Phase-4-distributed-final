import pyopencl as cl
import numpy as np

def convert_to_greyscale(ctx, queue, channel, height, width, greyscale_kernel):
    """
    Convert a single channel to greyscale using OpenCL
    """
    channel_flat = channel.reshape(-1)
    empty_array = np.empty_like(channel_flat)

    channel_buffer = cl.Buffer(ctx, cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR, hostbuf=channel_flat)
    result_buffer = cl.Buffer(ctx, cl.mem_flags.WRITE_ONLY, size=empty_array.nbytes)

    program = cl.Program(ctx, greyscale_kernel).build()
    program.greyscale(queue, (height * width,), None, channel_buffer, result_buffer)

    result = np.empty_like(channel_flat)
    cl.enqueue_copy(queue, result, result_buffer).wait()

    return result.reshape(height, width)

def apply_intensity_kernel(ctx, queue, channel, value, bright_kernel, dark_kernel):
    """
    Apply brightness/darkness adjustment to a single channel using OpenCL kernel
    """
    channel_flat = channel.reshape(-1)
    empty_array = np.empty_like(channel_flat)

    channel_buffer = cl.Buffer(ctx, cl.mem_flags.READ_WRITE | cl.mem_flags.COPY_HOST_PTR, hostbuf=channel_flat)
    result_buffer = cl.Buffer(ctx, cl.mem_flags.WRITE_ONLY, size=empty_array.nbytes)

    if value == 1:
        program = cl.Program(ctx, bright_kernel).build()
        program.bright(queue, channel_flat.shape, None, channel_buffer)
    else:
        program = cl.Program(ctx, dark_kernel).build()
        program.dark(queue, channel_flat.shape, None, channel_buffer)

    result = np.empty_like(channel_flat)
    cl.enqueue_copy(queue, result, channel_buffer).wait()

    return result.reshape(channel.shape)

def threshold_helper(ctx, queue, data, threshold_kernel, original_height, original_width):
    """
    Apply thresholding operation using OpenCL
    """
    buffer_data = cl.Buffer(ctx, cl.mem_flags.READ_WRITE, size=data.nbytes)
    cl.enqueue_copy(queue, buffer_data, data)

    program = cl.Program(ctx, threshold_kernel).build()
    program.Thresh(queue, data.shape, (1,), buffer_data)

    cl.enqueue_copy(queue, data, buffer_data)
    thresholded_image = data.reshape((original_height, original_width))
    thresholded_image = thresholded_image.astype(np.uint8)

    return thresholded_image
