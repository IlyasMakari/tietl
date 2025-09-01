rule AlienCombolist
{
    meta:
        description = "Matches ULP files from the alien chat"

    condition:
        (
            filename matches /^@TXTLOG_ALIEN.*\.txt$/i or
            filename matches /^@TXT_ALIEN.*\.txt$/i
        )
}
