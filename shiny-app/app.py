from shiny import App, render, ui
import sys
import os

# Add shared module to path
sys.path.append('/app/shared')
from demo_queries import IcebergDemoQueries

app_ui = ui.page_fluid(
    ui.h1("🚀 Simple Trino Demo with Shared Module"),
    ui.input_action_button("run_test", "Test Shared Module", class_="btn-primary"),
    ui.br(),
    ui.input_action_button("catalog_test", "Show Catalog Contents", class_="btn-success"),
    ui.br(),
    ui.br(),
    ui.output_text_verbatim("result")
)


def server(input, output, session):
    
    @output
    @render.text
    def result():
        if input.catalog_test():
            try:
                # Test catalog contents using shared module
                queries = IcebergDemoQueries()
                
                # Get current data query
                query_sql, description = queries.story_current_data()
                
                # Execute it
                import trino
                conn = trino.dbapi.connect(
                    host="trino",
                    port=8080,
                    user="admin",
                    catalog="iceberg"
                )
                cursor = conn.cursor()
                cursor.execute(query_sql)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                cursor.close()
                conn.close()
                
                # Format results nicely
                result_text = f"✅ Catalog Contents Retrieved!\n\n"
                result_text += f"Query: {query_sql}\n\n"
                result_text += f"Description: {description}\n\n"
                result_text += f"Results ({len(results)} rows):\n"
                result_text += f"Columns: {', '.join(columns)}\n\n"
                
                for i, row in enumerate(results[:5]):  # Show first 5 rows
                    result_text += f"Row {i+1}: {row}\n"
                
                if len(results) > 5:
                    result_text += f"... and {len(results) - 5} more rows"
                
                return result_text
            
            except Exception as e:
                return f"❌ Catalog Error: {str(e)}"
                
        elif input.run_test():
            try:
                # Test the shared module
                queries = IcebergDemoQueries()
                
                # Get a simple query
                query_sql, description = queries.connectivity_test()
                
                # Execute it
                import trino
                conn = trino.dbapi.connect(
                    host="trino",
                    port=8080,
                    user="admin",
                    catalog="iceberg"
                )
                cursor = conn.cursor()
                cursor.execute(query_sql)
                result = cursor.fetchone()
                cursor.close()
                conn.close()
                
                return f"✅ Shared Module Works!\nQuery: {query_sql}\nResult: {result}\nDescription: {description}"
            
            except Exception as e:
                return f"❌ Error: {str(e)}"
        else:
            return "Click 'Test Shared Module' or 'Show Catalog Contents' to see results"


app = App(app_ui, server)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)