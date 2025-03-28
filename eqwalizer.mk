EQWALIZER_RELEASE_VSN := 2024-11-07
EQWALIZER_OTP_VSN := 26.2

ERLANG_MK_TMP = $(shell TERM=dumb QUIET=1 REBAR_PROFILE=eqwalizer rebar3 path --base)
EQWALIZER_TMP := $(ERLANG_MK_TMP)/eqwalizer
EQWALIZER_ELP := $(EQWALIZER_TMP)/elp

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_M)-$(UNAME_S),arm64-Darwin)
EQWALIZER_FLAVOUR := macos-aarch64-apple-darwin
else ifeq ($(UNAME_M)-$(UNAME_S),x86_64-Darwin)
EQWALIZER_FLAVOUR := macos-x86_64-apple-darwin
else ifeq ($(UNAME_M)-$(UNAME_S),x86_64-Linux)
EQWALIZER_FLAVOUR := linux-x86_64-unknown-linux-gnu
else
$(error TODO $(UNAME_M)-$(UNAME_S))
endif

EQWALIZER_TAR_GZ := $(EQWALIZER_TMP)/elp-$(EQWALIZER_FLAVOUR)-otp-$(EQWALIZER_OTP_VSN).tar.gz

eqwalize:: $(EQWALIZER_ELP)
	REBAR_PROFILE=eqwalizer $(EQWALIZER_ELP) eqwalize-all

$(EQWALIZER_TAR_GZ):
	mkdir -p $(EQWALIZER_TMP)
	curl -s -L -o $(EQWALIZER_TAR_GZ) https://github.com/WhatsApp/erlang-language-platform/releases/download/$(EQWALIZER_RELEASE_VSN)/elp-$(EQWALIZER_FLAVOUR)-otp-$(EQWALIZER_OTP_VSN).tar.gz

$(EQWALIZER_ELP): $(EQWALIZER_TAR_GZ)
	tar -C $(EQWALIZER_TMP) -xz -f $(EQWALIZER_TAR_GZ)
	# Set the tarball to match the binary, so we don't keep unpacking it.
	touch -r $(EQWALIZER_ELP) $(EQWALIZER_TAR_GZ)
