from bounded_buffer import BoundedBuffer

lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi sit amet vulputate lorem. Integer euismod odio id arcu ullamcorper euismod. Etiam dignissim porta venenatis. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque eu semper purus. Quisque dictum urna metus, consectetur aliquet nunc eleifend non. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce eu luctus ipsum. Aenean tempus tristique purus, a condimentum ipsum. Morbi feugiat nisi ipsum, quis fringilla dui molestie ultricies. Sed vulputate interdum quam, eu sollicitudin tellus iaculis eget. Fusce sed erat purus. Sed fermentum euismod semper. Pellentesque convallis lectus non purus tristique aliquet. Vivamus quis nulla ac sapien placerat tincidunt eleifend vitae risus."

# means 3 caches at a time with max local size = 2
buffer = BoundedBuffer(5, 2)

for i in range(0, 5):
    for j in range(0, 3):
        buffer.set(i, j, "Hello world => {}".format(lorem))


