import streamlit as st


def main() -> None:
    st.set_page_config(page_title="Visa Atlas", page_icon="🌍", layout="wide")
    st.title("Visa Atlas")
    st.caption("Reinvent the wheel to check H1B/PERM sponsorships.")

    st.write(
        "This is the initial Streamlit app scaffold. "
        "Use it as the starting point for search, filters, and sponsorship insights."
    )

    st.subheader("Next steps")
    st.markdown(
        """
        - Connect your data source.
        - Add employer and visa filters.
        - Visualize sponsorship trends.
        """
    )


if __name__ == "__main__":
    main()
