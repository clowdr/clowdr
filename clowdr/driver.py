#!/usr/bin/env python

from argparse import ArgumentParser
import sys


def dev(tool, invocation, location, **kwargs):
    print(tool, invocation, location, kwargs)
    return 0


def deploy(tool, invocation, location, auth, **kwargs):
    print(tool, invocation, location, auth, kwargs)
    return 0


def share(location, **kwargs):
    print(location, kwargs)
    return 0


def main(args=None):
    desc = "Interface for launching Boutiques task locally and in the cloud"
    parser = ArgumentParser("Clowdr CLI", description=desc)
    parser.add_argument("--verbose", "-v", action="store_true")
    subparsers = parser.add_subparsers(help="Modes of operation", dest="mode")

    parser_dev = subparsers.add_parser("dev")
    parser_dev.add_argument("tool", help="boutiques descriptor for a tool")
    parser_dev.add_argument("invocation", help="input(s) for the tool")
    parser_dev.add_argument("location", help="local or s3 location for clowdr")

    parser_dpy = subparsers.add_parser("deploy")
    parser_dpy.add_argument("tool",  help="boutiques descriptor for a tool")
    parser_dpy.add_argument("invocation", help="input(s) for the tool")
    parser_dpy.add_argument("location", help="local or s3 location for clowdr")
    parser_dpy.add_argument("auth", help="credentials for the remote resource")

    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("location", help="local or s3 location for clowdr")

    inps = parser.parse_args(args) if args is not None else parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    mode = inps.mode
    del inps.mode

    if mode == "dev":
        dev(**vars(inps))
    elif mode == "deploy":
        deploy(**vars(inps))
    elif mode == "share":
        share(**vars(inps))


if __name__ == "__main__":
    main()

