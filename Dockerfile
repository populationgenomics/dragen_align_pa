FROM australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_hail_gcloud:0.2.134.cpg1

# Dragen align pa pipeline version.
ENV VERSION=1.0.2

ARG ICA_CLI_VERSION=${ICA_CLI_VERSION:-2.34.0}

RUN apt update && apt install -y \
    unzip \
    git \
    jq && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/* && \
    # Download and check the ICA CLI.
    wget -q https://stratus-documentation-us-east-1-public.s3.amazonaws.com/cli/${ICA_CLI_VERSION}/ica-linux-amd64.sha256 -O /tmp/ica-linux-amd64.sha256 && \
    wget -q https://stratus-documentation-us-east-1-public.s3.amazonaws.com/cli/${ICA_CLI_VERSION}/ica-linux-amd64.zip -O /tmp/ica-linux-amd64.zip && \
    sed -i 's|/home/ec2-user/workspace/ontrolPlane_ICA_CLI_release_2.34/target/ica-linux-amd64.zip|/tmp/ica-linux-amd64.zip|' /tmp/ica-linux-amd64.sha256 && \
    sha256sum -c /tmp/ica-linux-amd64.sha256 && \
    unzip -d /tmp /tmp/ica-linux-amd64.zip && \
    cp /tmp/linux-amd64/icav2 /usr/local/bin/icav2 && \
    chmod a+x /usr/local/bin/icav2 && \
    rm -rf /tmp/ica-linux-amd64.sha256 /tmp/ica-linux-amd64.zip /tmp/linux-amd64


WORKDIR /dragen_align_pa

# Add in the additional requirements that are most likely to change.
COPY LICENSE pyproject.toml README.md .
COPY src src/
COPY third_party /third_party


RUN pip install git+https://github.com/Illumina/ica-sdk-python.git \
    && pip install . \
    && pip freeze
