import pandas as pd
import streamlit as st
import plotly.express as px

# Load data
df = pd.read_csv('/Users/virtrivedi/C++/Sean/python/overall_correlations.csv', names=['symbol1', 'symbol2', 'overall_correlation'], skiprows=1)
df['overall_correlation'] = pd.to_numeric(df['overall_correlation'], errors='coerce')

# Get unique symbols
symbols = pd.unique(df[['symbol1', 'symbol2']].values.ravel())
symbols = sorted([s for s in symbols if pd.notnull(s)])

# Set a max limit
MAX_SELECTION = 30

# Sidebar multiselect
selected_symbols = st.multiselect(
    f"Select up to {MAX_SELECTION} symbols to compare:",
    symbols,
    default=symbols[:10]
)

# Show warning if over limit
if len(selected_symbols) > MAX_SELECTION:
    st.error(f"You've selected {len(selected_symbols)} symbols. Please select {MAX_SELECTION} or fewer.")
else:
    if selected_symbols:
        filtered_df = df[df['symbol1'].isin(selected_symbols) & df['symbol2'].isin(selected_symbols)]

        # Pivot table
        pivot_df = filtered_df.pivot(index='symbol1', columns='symbol2', values='overall_correlation')
        pivot_df = pivot_df.combine_first(pivot_df.T)

        # Reindex to keep order
        pivot_df = pivot_df.reindex(index=selected_symbols, columns=selected_symbols)

        # Create heatmap
        fig = px.imshow(
            pivot_df,
            color_continuous_scale='RdBu',
            zmin=-1,
            zmax=1,
            aspect='equal',
            labels=dict(x="Symbol", y="Symbol", color="Correlation"),
        )

        # Adjust layout to ensure symbols show up on both axes
        fig.update_layout(
            title="Correlation Heatmap",
            xaxis_title="Symbol",
            yaxis_title="Symbol",
            xaxis_showgrid=False,
            yaxis_showgrid=False,
            xaxis=dict(tickmode='array', tickvals=list(range(len(selected_symbols))), ticktext=selected_symbols),
            yaxis=dict(tickmode='array', tickvals=list(range(len(selected_symbols))), ticktext=selected_symbols),
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Please select at least one symbol.")