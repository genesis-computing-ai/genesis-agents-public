from setuptools import setup, find_namespace_packages

setup(
    name="genesis_bots",
    description="Genesis Bots Package",
    packages=find_namespace_packages(include=[
        'genesis_bots',
        'genesis_bots.*',
    ]),
    package_dir={
        "": ".",
    },
    package_data={
        'genesis_bots': [
            '**/*.yaml',
            '**/*.conf',
            '**/*.json',
            '**/*.md',
            '**/LICENSE',
            '**/*.sqlite',
            '**/*.sql',
            '**/*.db',
            'apps/streamlit_gui/**/*',
            'apps/streamlit_gui/*.png',
            'apps/streamlit_gui/*',
            'requirements.txt',
            'default_config/*',
            'genesis_sample_golden/**/*',
            'genesis_sample_golden/demo_data/*',
            'genesis_sample_golden/demo_data/*.sqlite',
        ],
    },
    zip_safe=False,
    include_package_data=True,
)