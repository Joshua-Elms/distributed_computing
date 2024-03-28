from pathlib import Path
from time import perf_counter


def serial_mapreduce(sections, map_f, reduce_f, reduce_base_type):
    split_sections = [section.split() for section in sections]
    out = []
    for i, split_section in enumerate(split_sections):
        for k in split_section:
            out.append(map_f(i, k))

    result = {k: reduce_base_type for k, v in out}
    for k, v in out: 
        result[k] = reduce_f(result[k], v)


    # to string
    items = []
    for k, v in result.items():
        items.append(f"{k}:{v}")

    return " ".join(items)


def write_output(out, output_path):
    with open(output_path, "w") as f:
        f.write(out)


def main(input_dir: Path, output_path: Path, map_f, reduce_f, reduce_base_type, n_iters: int = 1):
    sections = []
    for f in input_dir.glob("*.txt"):
        with open(f, "r") as f_in:
            sections.append(f_in.read())

    start = perf_counter()
    for _ in range(n_iters):
        out = serial_mapreduce(sections, map_f, reduce_f, reduce_base_type)
    stop = perf_counter()
    avg_runtime = (stop - start) / n_iters

    write_output(out, output_path)

    return avg_runtime


if __name__ == "__main__":
    avg_t_wc = main(Path("data/test_letters"), Path("output/serial_WC.txt"),
                    lambda d, x: (x, 1), lambda x, y: x + y, reduce_base_type=int())
    avg_t_ii = main(Path("data/test_letters"), Path("output/serial_II.txt"),
                    lambda d, x: (x, d), lambda x, y: x.union({y}), reduce_base_type=set())
