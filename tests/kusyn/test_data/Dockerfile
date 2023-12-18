FROM ubuntu

COPY single_source /dest
ENV PYTHONPYCACHEPREFIX=/tmp/pycache
WORKDIR /app

COPY source1 source_with_type.lock .hidden_source source_folder/ /dest_multy/
RUN echo 'wassup'

COPY more_sources/ /dest3
