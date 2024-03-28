import re

with open("wuthering_heights.txt", "r") as f:
    raw = f.read()

divchar = "CHAPTER"
pattern = divchar + r" [A-Z]{1,}"
splitchars = [".", ",", "!", "?", ":", ";", "(", ")", '”', "“", "—", r"\t", "_", '-', "‘"]
data = re.split(pattern, raw)
data = [chapter.strip() for chapter in data]
final_data = ["" for _ in range(len(data))]
for i, chapter in enumerate(data):
    for char in splitchars:
        chapter = chapter.replace(char, " ")
        chapter = chapter.replace("’", "")
        chapter = " ".join(chapter.split())

    final_data[i] = chapter.lower()

for i in range(1, len(final_data)):
    with open(f"wuthering_heights/chapter_{i}.txt", "w") as f:
        f.write(final_data[i])

print(len(final_data))
