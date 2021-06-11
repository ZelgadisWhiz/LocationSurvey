Imports System.Text.RegularExpressions

Public Class LocationSurvey

    Private WellNo As String
    Private LSD As UShort
    Private Section As UShort
    Private Township As UShort
    Private Range As UShort
    Private Meridian As UShort
    Private evt As UShort
    Private wellNoPattern As String = "\w*\/"
    Private numberPattern As String = "\d*"
    Private eventPattern As String = "\/\/\d\d"

    Sub New()

    End Sub

    Private Sub ParseDLS(DLS As String)

        Dim temp As String
        temp = Regex.Match(DLS, wellNoPattern).ToString
        If Not String.IsNullOrEmpty(temp) Then
            WellNo = temp.Substring(0, temp.Length - 1)
        End If
        DLS = Regex.Replace(DLS, wellNoPattern, String.Empty).ToString

        If Regex.Match(DLS, eventPattern).Success Then
            UShort.TryParse(Regex.Match(DLS, eventPattern).Value.Substring(2), evt)
        End If

        Dim patternIndex As Integer = 0

        For Each Match As Match In Regex.Matches(DLS, numberPattern)

            If IsNumeric(Match.Value) Then

                If patternIndex = 0 Then
                    UShort.TryParse(Match.Value, LSD)
                ElseIf patternIndex = 1 Then
                    UShort.TryParse(Match.Value, Section)
                ElseIf patternIndex = 2 Then
                    UShort.TryParse(Match.Value, Township)
                ElseIf patternIndex = 3 Then
                    UShort.TryParse(Match.Value, Range)
                ElseIf patternIndex = 4 Then
                    UShort.TryParse(Match.Value, Meridian)
                End If
                patternIndex += 1
            End If
        Next
    End Sub

    Function IsValid() As Boolean

        If Not (IsNothing(LSD) OrElse IsNothing(Section) OrElse IsNothing(Township) OrElse IsNothing(Range) OrElse IsNothing(Meridian)) Then
            Return True
        Else
            Return False
        End If

    End Function

    Public Sub SaveDLS(db As EnerTraxEntities, DLS As String, isTopHole As Boolean, IDWell As Guid)

        If Not String.IsNullOrEmpty(DLS) Then
            Dim existingDLS = db.udp_DLS_SelectByWell(IDWell, isTopHole).FirstOrDefault
            Me.ParseDLS(DLS)

            If Me.IsValid() Then

                If existingDLS Is Nothing Then
                    db.udp_DLS_Insert(WellNo, LSD, Section, Township, Range, Meridian, evt, isTopHole, IDWell)
                Else
                    db.udp_DLS_Update(WellNo, LSD, Section, Township, Range, Meridian, evt, isTopHole, IDWell)
                End If

            End If
        End If

    End Sub

    Function GetWellNo() As String

        If TypeOf WellNo Is String Then
            Return WellNo
        Else
            Return ""
        End If

    End Function

    Function GetLSD() As UShort
        Return LSD
    End Function

    Function GetSection() As UShort
        Return Section
    End Function

    Function GetTownship() As UShort
        Return Township
    End Function

    Function GetRange() As UShort
        Return Range
    End Function

    Function GetMeridian() As UShort
        Return Meridian
    End Function

End Class
