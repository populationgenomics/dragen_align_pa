FROM australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_hail_gcloud:0.2.134.cpg1

# Dragen align pa pipeline version.
ENV VERSION=2.1.0

ARG ICA_CLI_VERSION="2.39.0"
ARG SOMALIER_VERSION="0.3.1"

RUN apt update && apt install -y \
    unzip \
    git \
    procps \
    jq && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    # Download and check the ICA CLI.
    wget -q https://stratus-documentation-us-east-1-public.s3.amazonaws.com/cli/${ICA_CLI_VERSION}/ica-linux-amd64.sha256 -O /tmp/ica-linux-amd64.sha256 && \
    wget -q https://stratus-documentation-us-east-1-public.s3.amazonaws.com/cli/${ICA_CLI_VERSION}/ica-linux-amd64.zip -O /tmp/ica-linux-amd64.zip && \
    sed -i 's|target/ica-linux-amd64.zip|/tmp/ica-linux-amd64.zip|' /tmp/ica-linux-amd64.sha256 && \
    sha256sum -c /tmp/ica-linux-amd64.sha256 && \
    unzip -d /tmp /tmp/ica-linux-amd64.zip && \
    cp /tmp/linux-amd64/icav2 /usr/local/bin/icav2 && \
    chmod a+x /usr/local/bin/icav2 && \
    rm -rf /tmp/ica-linux-amd64.sha256 /tmp/ica-linux-amd64.zip /tmp/linux-amd64


WORKDIR /dragen_align_pa

# Add in the additional requirements that are most likely to change.
COPY LICENSE pyproject.toml README.md ./
COPY src src/
COPY third_party third_party/


RUN pip install git+https://github.com/Illumina/ica-sdk-python.git \
    && pip install . \
    && pip install third_party/popgen_cli-2.1.0-py3-none-any.whl \
    && pip install typing-extensions==4.13.0 \
    && pip install multiqc==1.30

RUN wget https://github.com/brentp/somalier/releases/download/v${SOMALIER_VERSION}/somalier && \
    chmod +x somalier && \
    mv somalier /usr/local/bin/
