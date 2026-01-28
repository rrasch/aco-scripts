%{!?git_tag:%{error:git_tag macro must be defined}}
%{!?git_commit:%{error:git_commit macro must be defined}}

%global name aco-scripts
%global version %(echo %{git_tag} | sed 's/^v//')
%global release 1.dlts.git%{git_commit}%{?dist}

%global git_url https://github.com/rrasch/%{name}
%global install_dir /usr/local/dlib/%{name}

Name:           %{name}
Version:        %{version}
Release:        %{release}
Summary:        Utility scripts for Arabic Collections Online (ACO)
License:        MIT
URL:            %{git_url}
BuildArch:      noarch
BuildRequires:  git
Requires:       python3

%description
%{name} is a collection of utility scripts used in support of
Arabic Collections Online (ACO) workflows, including processing,
validation, and management of digitized collection data.

%prep
rm -rf %{name}*

git clone %{git_url} %{name}-%{version}
cd %{name}-%{version}
git -c advice.detachedHead=false checkout %{git_tag}

%build
:

%install
rm -rf %{buildroot}
cd %{name}-%{version}

install -d %{buildroot}%{install_dir}
cp -a * %{buildroot}%{install_dir}/
rm -rf %{buildroot}%{install_dir}/.git

find %{buildroot}%{install_dir} -type f -name "*.py" -exec chmod 0755 {} \;

%check
# Sanity check: ensure Python files parse
for f in $(find %{buildroot}%{install_dir} -name "*.py"); do
    %{python3} -m py_compile "$f"
done
rm -rf %{buildroot}%{install_dir}/__pycache__

%files
%{install_dir}

%changelog
