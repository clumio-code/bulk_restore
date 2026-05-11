#
# Copyright 2024 Clumio, a Commvault company.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# SHELL ensures more consistent behavior between macOS and Linux.
SHELL=/bin/bash

test_reports := build/test_reports/py
VERSION := $(shell cat VERSION)
# SAM-provided Amazon Linux 2023 image with Python 3.12 + pip. Used to install
# Lambda deps with the right linux/x86_64 wheels (e.g. pydantic-core).
LAMBDA_BUILD_IMAGE := public.ecr.aws/sam/build-python3.12:latest-x86_64

.PHONY: *


clean:
	rm -rf build .mypy_cache .coverage *.egg-info dist code/.coverage

build:
	rm -rf build/lambda build/clumio_bulk_restore*.zip build/*_cft.yaml
	mkdir -p build/lambda
	mkdir -p build/lambda/utils
	cp code/*.py build/lambda/
	cp -r code/utils/* build/lambda/utils
	cp VERSION build/lambda/version.txt
	# Install Python deps inside a Linux x86_64 container so native binaries
	# (e.g. pydantic-core's Rust wheel) match the Lambda runtime. Requires Docker.
	docker run --rm \
		--user $$(id -u):$$(id -g) \
		-v "$(CURDIR):/work" \
		-w /work \
		$(LAMBDA_BUILD_IMAGE) \
		pip install --no-cache-dir --target build/lambda -r requirements.txt
	cd build/lambda && zip -r ../clumio_bulk_restore-$(VERSION).zip .
	sed 's/__BULK_RESTORE_VERSION__/$(VERSION)/g' \
		code/clumio_bulk_deploy_cft.yaml > build/clumio_bulk_deploy_cft.yaml
	@echo "Built bulk_restore version $(VERSION) -> build/clumio_bulk_restore-$(VERSION).zip"

# Install the dependencies locally.
install:
	pip install -r requirements.txt


# Install the development dependencies.
install-dev:
	pip install -r requirements-dev.txt


# Run the unittests.
test:
	rm -rf $(test_reports) .coverage code/.coverage; \
    mkdir -p $(test_reports); \
    PYTHONPATH=code python3 -m green -v --run-coverage \
      --junit-report=$(test_reports)/bulk_restore-pytests.xml .; \
    python3 -m coverage xml -o $(test_reports)/bulk_restore-pycoverage.xml; \
    python3 -m coverage html -d $(test_reports)/bulk_restore-pycoverage-html
	@echo "HTML code coverage report was generated in $(test_reports)/bulk_restore-pycoverage-html"
	@echo "Open it with:"
	@echo "  open $(test_reports)/bulk_restore-pycoverage-html/index.html"


lint:
	ruff check


format:
	ruff format


format-check:
	ruff format --check


mypy:
	mypy code
