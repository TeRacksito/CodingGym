# CodingGy

A **daemon Python service** that processes Java programming exercise data to ensure compilation, execution, and testing.

Requires DAW Bot to work properly.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

- It's recommended to use Virtual Environment and run `pip install -r requirements.txt`.

- It's needed **geckodriver** on the working directory.

- Also you need two important files.

    - creds.txt

        Contains credentials for the service. In the case of our specific web page, there is three elements, separated by line breaks. Username, Password and IPC secret key.
    
    - page_ids.json

        JSON parsable object that has an hash table with this structure:
        
        `{key (ID of the page): value (name of the page as string)}`

        This is on the repository.

## Usage

Once you have everything done, you can run bot.py, or you can create an Systemd service to handle the bot (intended way) / (.service files are not included).

If you want to use this simple code to just scrape some page, it's recommended to
perform some minor changes to remove all IPC code and control flow statements.
Without that, the code should create a data.json file with all data scraped from the
web page on it's own.

## Contributing

Not allowed.

## License

You can fork and work with it as you want, no complains.