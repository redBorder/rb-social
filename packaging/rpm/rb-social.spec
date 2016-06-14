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
mkdir -p %{buildroot}/usr/share/%{name}
install -D -m 644 target/rb-social-*-selfcontained.jar %{buildroot}/usr/share/%{name}

%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(644,root,root)
/usr/share/%{name}/rb-social-*-selfcontained.jar

%changelog
* Tue Jun 14 2016 Carlos J. Mateos  <cjmateos@redborder.com> - 1.0.0-1
- first spec version
