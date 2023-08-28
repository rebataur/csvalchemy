import solara

@solara.component
def Page():
    with solara.Column():
        solara.Title("I'm in the browser tab and the toolbar")
        with solara.Sidebar():
            solara.Markdown("## I am in the sidebar")
            solara.SliderInt(label="Ideal for placing controls")
        solara.Info("I'm in the main content area, put your main content here")
        with solara.Card("Use solara.Columns([1, 2]) to create relatively sized columns"):
            with solara.Columns([1, 2]):
                solara.Success("I'm in the first column")
                solara.Warning("I'm in the second column, I am twice as wide")
                solara.Info("I am like the first column")

        with solara.Card("Use solara.Column() to create a full width column"):
            with solara.Column():
                solara.Success("I'm first in this full with column")
                solara.Warning("I'm second in this full with column")
                solara.Error("I'm third in this full with column")

        with solara.Card("Use solara.ColumnsResponsive(6, large=4) to response to screen size"):
            with solara.ColumnsResponsive(6, large=4):
                for i in range(6):
                    solara.Info("two per column on small screens, three per column on large screens")