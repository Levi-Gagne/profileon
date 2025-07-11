# profileon/src/config/cla_color_config.py

class CLAColor:
    """
    CLA Brand Colors for digital output.
    Provides both ANSI escape codes for CLI and a helper to convert to HEX for HTML/Streamlit.
    Usage:
        # CLI: print(f"{CLA.cla['riptide']}Hello{CLA.r}")
        # HTML: color=CLA.ansi_to_hex(CLA.cla['riptide'])
    """

    cla = {
        # --- Primary ---
        "riptide":         "\033[38;2;125;210;211m",  # #7DD2D3
        "navy":            "\033[38;2;46;51;78m",     # #2E334E

        # --- Secondary ---
        "celadon":         "\033[38;2;226;232;104m",  # #E2E868
        "saffron":         "\033[38;2;251;197;90m",   # #FBC55A
        "scarlett":        "\033[38;2;238;83;64m",    # #EE5340

        # --- Neutrals ---
        "charcoal":        "\033[38;2;37;40;42m",     # #25282A
        "smoke":           "\033[38;2;171;174;171m",  # #ABAEAB
        "cloud":           "\033[38;2;247;247;246m",  # #F7F7F6
        "white":           "\033[38;2;255;255;255m",  # #FFFFFF
        "black":           "\033[38;2;0;0;0m",        # #000000

        # --- Tints ---
        "riptide_tints": {
            "light":        "\033[38;2;194;234;234m",  # #C2EAEA
            "medium":       "\033[38;2;164;223;224m",  # #A4DFE0
            "dark":         "\033[38;2;149;217;219m",  # #95D9DB
        },
        "celadon_tints": {
            "light":        "\033[38;2;245;247;209m",  # #F5F7D1
            "medium":       "\033[38;2;238;242;178m",  # #EEF2B2
            "dark":         "\033[38;2;231;236;147m",  # #E7EC93
        },
        "saffron_tints": {
            "light":        "\033[38;2;254;238;206m",  # #FEEECE
            "medium":       "\033[38;2;253;220;156m",  # #FDDC9C
            "dark":         "\033[38;2;252;209;123m",  # #FCD17B
        },
        "scarlett_tints": {
            "light":        "\033[38;2;251;205;196m",  # #FBCDC4
            "medium":       "\033[38;2;246;155;137m",  # #F69B89
            "dark":         "\033[38;2;243;121;98m",   # #F37962
        },

        # --- Shades ---
        "riptide_shades": {
            "light":        "\033[38;2;73;191;193m",   # #49BFC1
            "medium":       "\033[38;2;57;165;167m",   # #39A5A7
            "dark":         "\033[38;2;36;120;122m",   # #24787A
        },
        "navy_shades": {
            "light":        "\033[38;2;38;42;64m",     # #262A40
            "medium":       "\033[38;2;30;33;51m",     # #1E2133
            "dark":         "\033[38;2;23;25;39m",     # #171927
        },
    }

    # ANSI reset
    r = "\033[0m"

    @staticmethod
    def cla_color(name, shade=None):
        """
        Quick access to CLA colors and their tints/shades.
        Example:
            CLA.cla_color('navy')
            CLA.cla_color('scarlett', 'light')
            CLA.cla_color('riptide_shades', 'dark')
        """
        c = CLA.cla
        if shade and name + "_tints" in c:
            return c[name + "_tints"].get(shade, "")
        if shade and name + "_shades" in c:
            return c[name + "_shades"].get(shade, "")
        return c.get(name, "")

    @staticmethod
    def ansi_to_hex(ansi_code):
        """
        Converts ANSI 24-bit color escape string to HEX, e.g.:
        '\\033[38;2;125;210;211m' => '#7DD2D3'
        """
        if not ansi_code.startswith("\033"):
            return ansi_code  # Already HEX or plain
        # Extract numbers: e.g. '\033[38;2;125;210;211m'
        parts = ansi_code.strip("\033[m").split(";")
        # Find the R,G,B values (usually parts 1,2,3 after "38;2")
        try:
            idx = parts.index('2')
            r, g, b = map(int, parts[idx+1:idx+4])
            return '#%02X%02X%02X' % (r, g, b)
        except Exception:
            return "#000000"

# Example usage:
# For CLI:   print(f"{CLA.cla['riptide']}Hello CLA{CLA.r}")
# For HTML:  color=CLA.ansi_to_hex(CLA.cla['riptide'])
# In f-strings: f"<div style='color:{CLA.ansi_to_hex(CLA.cla['navy'])}'>...</div>"
