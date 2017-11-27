FROM centos/s2i-base-centos7

##################  ANTS CONTAINERIZATION ##################



MAINTAINER Miheer "misalunk@redhat.com"

RUN yum install -y cmake && \
    yum install -y make && \
    yum install -y git && \
    cd $HOME && \
    git clone https://github.com/ANTsX/ANTs.git && \
    cd ANTs && \
    git checkout tags/v2.2.0 && \
    mkdir bin && \
    cd bin && \
    mkdir ants && \
    cd ants && \
    echo "Starting ccmake" && \
    ls -la && \
    export TERM=xterm && \
    cmake $HOME/ANTs && \
    echo "end ccmake" && \
    make -j 10 && \
    cp -r ~/ANTs/Scripts/. ~/ANTs/bin/ants/bin
    

ENV ANTSPATH=${HOME}/ANTs/bin/ants/bin/ 

ENV PATH=${ANTSPATH}:$PATH
 




#######################################################



############# PYTHON INSTALLATION ################################


EXPOSE 8080

ENV PYTHON_VERSION=3.6 \
    PATH=$HOME/.local/bin/:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8 \
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    PIP_NO_CACHE_DIR=off

ENV SUMMARY="Platform for building and running Python $PYTHON_VERSION applications" \
    DESCRIPTION="Python $PYTHON_VERSION available as docker container is a base platform for \
building and running various Python $PYTHON_VERSION applications and frameworks. \
Python is an easy to learn, powerful programming language. It has efficient high-level \
data structures and a simple but effective approach to object-oriented programming. \
Python's elegant syntax and dynamic typing, together with its interpreted nature, \
make it an ideal language for scripting and rapid application development in many areas \
on most platforms."


RUN yum install -y centos-release-scl-rh && \
    yum-config-manager --enable centos-sclo-rh-testing && \
    INSTALL_PKGS="rh-python36 rh-python36-python-devel rh-python36-python-setuptools rh-python36-python-pip \
	 nss_wrapper httpd24 httpd24-httpd-devel httpd24-mod_ssl httpd24-mod_auth_kerb httpd24-mod_ldap \
         httpd24-mod_session atlas-devel gcc-gfortran libffi-devel libtool-ltdl enchant" && \
    yum install -y --setopt=tsflags=nodocs --enablerepo=centosplus $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    # Remove centos-logos (httpd dependency, ~20MB of graphics) to keep image
    # size smaller.
    rpm -e --nodeps centos-logos && \
    yum clean all -y



# - Create a Python virtual environment for use by any application to avoid
#   potential conflicts with Python packages preinstalled in the main Python
#   installation.
# - In order to drop the root user, we have to make some directories world
#   writable as OpenShift default security model is to run the container
#   under random UID.
RUN source scl_source enable rh-python36 && \
    virtualenv /opt/app-root && \
    chown -R 1001:0 /opt/app-root && \
    fix-permissions /opt/app-root && \
    rpm-file-permissions

USER 1001

# Set the default CMD to print the usage of the language image.
#CMD $STI_SCRIPTS_PATH/usage





################# INSTALLING PLUGIN ANTSREG   ###########################

RUN git clone https://github.com/FNNDSC/pl-antsreg.git && \
    cd pl-antsreg 

WORKDIR /opt/app-root/src/pl-antsreg/antsreg/

RUN pip install -r /opt/app-root/src/pl-antsreg/requirements.txt #&& \

CMD ["antsreg.py", "--json"]

