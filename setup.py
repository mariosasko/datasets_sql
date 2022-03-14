from setuptools import find_packages, setup


DESCRIPTION = "datasets_sql is an extension package of 🤗 Datasets package that provides support for executing arbitrary SQL queries on datasets."


TESTS_REQUIRE = [
    "pytest",
]

QUALITY_REQUIRE = ["black~=22.0", "flake8>=3.8.3", "isort>=5.0.0"]


EXTRAS_REQUIRE = {
    "dev": TESTS_REQUIRE + QUALITY_REQUIRE,
    "tests": TESTS_REQUIRE,
}

setup(
    name="datasets-sql",
    version="0.1.0",
    description=DESCRIPTION,
    long_description=open("README.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    keywords="deep learning",
    license="Apache",
    author="Mario Šaško",
    author_email="mariosasko777@gmail.com",
    url="https://github.com/mariosasko/datasets_sql",
    packages=find_packages(),
    python_requires=">=3.7.0",
    install_requires=["pyarrow>=7.0.0", "datasets", "duckdb", "sql-metadata"],
    extras_require=EXTRAS_REQUIRE,
)
