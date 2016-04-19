from setuptools import (
    setup,
    find_packages,
)

setup(
    name='azmq',
    author='Julien Kauffmann',
    author_email='julien.kauffmann@freelan.org',
    maintainer='Julien Kauffmann',
    maintainer_email='julien.kauffmann@freelan.org',
    version=open('VERSION').read().strip(),
    url='http://ereOn.github.io/azmq',
    description=(
        "An asyncio-native implementation of ZMTP (ZMQ)."
    ),
    long_description="""\
AZMQ is an implementation of ZMTP (the protocol behind ZMQ) using native Python
3 asyncio sockets.
""",
    packages=find_packages(exclude=[
        'tests',
    ]),
    install_requires=[],
    test_suite='tests',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Development Status :: 5 - Production/Stable',
    ],
)
