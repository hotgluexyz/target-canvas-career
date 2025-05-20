# target-canvas-career

`target-canvas-career` is a Singer target for the Instructure [Canvas LMS API](https://canvas.instructure.com/doc/api/index.html).

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

You can install this target directly from the GitHub repository using pipx:

```bash
pipx install git+https://github.com/hotgluexyz/target-canvas-career.git
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-canvas-career --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `target-canvas-career` by itself or in a pipeline like [hotglue](https://hotgluew.com/).

### Executing the Target Directly

```bash
target-canvas-career --version
target-canvas-career --help

# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-canvas-career --config /path/to/target-canvas-career-config.json
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
