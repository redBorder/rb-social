Name:     rb-social
Version:  %{__version}
Release:  %{__release}%{?dist}

License:  GNU AGPLv3
URL:  https://github.com/redBorder/rb-social
Source0: %{name}-%{version}.tar.gz

BuildRequires: maven java-devel

Summary: redborder social module
Group: Services/Monitoring
Requires: java

%description
%{summary}

%prep
%setup -qn %{name}-%{version}

%build
export MAVEN_OPTS="-Xmx512m -Xms256m -Xss10m -XX:MaxPermSize=512m" && mvn clean package

%install
mkdir -p %{buildroot}/usr/lib/%{name}
install -D -m 644 target/rb-social-*-selfcontained.jar %{buildroot}/usr/lib/%{name}
mv %{buildroot}/usr/lib/%{name}/rb-social-*-selfcontained.jar %{buildroot}/usr/lib/%{name}/rb-social.jar
install -D -m 644 rb-social.service %{buildroot}/usr/lib/systemd/system/rb-social.service

%clean
rm -rf %{buildroot}

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d / -s /sbin/nologin \
    -c "User of %{name} service" %{name}
exit 0

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(644,root,root)
/usr/lib/%{name}
/usr/lib/systemd/system/rb-social.service

%changelog
* Tue Jun 14 2016 Carlos J. Mateos  <cjmateos@redborder.com> - 1.0.0-1
- first spec version
