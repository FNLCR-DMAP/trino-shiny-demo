import sys
import trino
import pandas as pd
from shiny import App, render, ui, reactive

# Add shared module to path  
sys.path.append('/app/shared')
from demo_queries import IcebergDemoQueries

# Define available queries - keeping it simple with just the essentials
AVAILABLE_QUERIES = [
    {
        "id": "connectivity_test",
        "label": "🔗 Test Trino Connection",
        "method": "connectivity_test",
        "class": "btn-primary"
    },
    {
        "id": "warehouse_info",
        "label": "🏢 Show Warehouse Contents",
        "method": "warehouse_info",
        "class": "btn-info"
    }
]

app_ui = ui.page_fluid(
    ui.h1("🚀 Trino Demo"),
    ui.p("Click the buttons below to test the connection and "
         "explore the warehouse."),
    ui.hr(),
    
    # Generate buttons dynamically from AVAILABLE_QUERIES
    [ui.input_action_button(
        query["id"],
        query["label"],
        class_=query["class"]
    ) for query in AVAILABLE_QUERIES],
    
    ui.br(),
    ui.br(),
    
    # Two-column layout for query and results
    ui.row(
        ui.column(6,
                  ui.h4("🔍 SQL Query"),
                  ui.output_text_verbatim(
                      "query_display",
                      placeholder="Query will appear here when you click a button..."
                  )
        ),
        ui.column(6,
                  ui.h4("📊 Results"),
                  ui.output_text("results_status"),
                  ui.output_table("results_table")
        )
    )
)


def server(input, output, session):
    
    # Reactive values to store current query and results
    current_query = reactive.Value("")
    current_results_status = reactive.Value("")
    current_results_data = reactive.Value(None)
    
    def execute_query(query_sql, description):
        """Execute a query and update both query and results displays"""
        # Update query display
        query_text = f"📋 Description: {description}\n\n{query_sql}"
        current_query.set(query_text)
        
        try:
            conn = trino.dbapi.connect(
                host="trino",
                port=8080,
                user="admin",
                catalog="iceberg"
            )
            cursor = conn.cursor()
            cursor.execute(query_sql)
            results = cursor.fetchall()
            columns = ([desc[0] for desc in cursor.description]
                       if cursor.description else [])
            cursor.close()
            conn.close()
            
            # Update status
            status_text = f"✅ Query Executed Successfully! ({len(results)} rows)"
            current_results_status.set(status_text)
            
            # Create DataFrame for table display
            if results and columns:
                df = pd.DataFrame(results, columns=columns)
                current_results_data.set(df)
            else:
                # Empty result
                current_results_data.set(pd.DataFrame())
            
        except Exception as e:
            error_msg = f"❌ Query Error: {str(e)}"
            current_results_status.set(error_msg)
            current_results_data.set(pd.DataFrame())
    
    # Handle button clicks for each query
    @reactive.Effect
    def handle_connectivity_test():
        if input.connectivity_test():
            queries = IcebergDemoQueries()
            query_sql, description = queries.connectivity_test()
            execute_query(query_sql, description)
    
    @reactive.Effect
    def handle_warehouse_info():
        if input.warehouse_info():
            queries = IcebergDemoQueries()
            query_sql, description = queries.warehouse_info()
            execute_query(query_sql, description)
    
    @output
    @render.text
    def query_display():
        return current_query.get()
    
    @output
    @render.text
    def results_status():
        return current_results_status.get()
    
    @output
    @render.table
    def results_table():
        df = current_results_data.get()
        if df is not None and not df.empty:
            return df
        else:
            return pd.DataFrame({"Message": ["No data to display"]})


app = App(app_ui, server)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)