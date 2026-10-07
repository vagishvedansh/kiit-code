import re

with open("main.go", "r") as f:
    content = f.read()

content = content.replace(
    '"muse-spark":              "laguna-s-2.1-free",',
    '"muse-spark":              "laguna-s-2.1-free",\n\t"muse-spark-1.3":          "muse-spark-1.3-contributor-free",\n\t"muse-spark-1.3-contributor-free": "muse-spark-1.3-contributor-free",'
)

with open("main.go", "w") as f:
    f.write(content)
