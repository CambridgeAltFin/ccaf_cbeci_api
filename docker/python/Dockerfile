ARG PYTHON_VERSION
FROM python:3.9.0
SHELL ["/bin/bash", "-l", "-c"]

#####################################
# Set Timezone
#####################################

ARG TZ=UTC
ENV TZ ${TZ}

COPY setup.sh /usr/local/bin/
RUN set -eu && chmod +x /usr/local/bin/setup.sh

RUN apt-get update && apt-get install -y curl wget gnupg2 systemd gettext-base

RUN /usr/local/bin/python -m pip install --upgrade pip && /usr/local/bin/python -m pip install poetry

ENTRYPOINT ["/bin/bash", "-l", "-c", "setup.sh"]
