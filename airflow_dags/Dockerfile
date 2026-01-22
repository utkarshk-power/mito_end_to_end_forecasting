FROM astrocrpublic.azurecr.io/runtime:3.1-10
USER root
RUN apt-get update && apt-get install -y git
USER astro
RUN pip install dvc